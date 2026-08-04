from __future__ import annotations

import asyncio
import functools
from typing import Any

from .._transport import (
    _STREAMING_OUT,
    GrpcConfig,
    GrpcSandboxRPC,
    _ConnectError,
    _RouteNotFoundError,
    auth_metadata,
    connect_unary_blocking,
    grpc_target,
    http_base_url,
)
from ..errors import map_rpc_error

# _STREAMING_OUT (shared with the sync transport): server-/bidi-streaming RPCs
# return an async-iterable call object (consumed via `async for`), so they cannot
# be `async def` (which would return a coroutine). Everything else — including
# client-streaming RPCs like WriteFileStream (async-iterator request in, single
# response out) — is awaited.


class AsyncGrpcSandboxRPC:
    """grpc.aio-backed control-plane transport.

    Mirrors the sync ``GrpcSandboxRPC`` surface: unary methods are awaitable,
    streaming methods return an async iterator, and the SSHGatewayClientService
    RPCs go over the Connect protocol (reusing the shared blocking helper on a
    thread) with the same legacy-service fallback.
    """

    _methods = dict(GrpcSandboxRPC._methods)
    _ssh_methods = dict(GrpcSandboxRPC._ssh_methods)

    _SSH_SERVICE = "tenki.sandbox.v1.SSHGatewayClientService"
    _SSH_LEGACY_SERVICE = "tenki.sandbox.v1.SSHGatewayService"

    def __init__(self, config: GrpcConfig) -> None:
        target, secure = grpc_target(config.base_url)
        self._target = target or config.base_url
        self._secure = secure
        self._http_base = http_base_url(config.base_url)
        self._metadata = auth_metadata(config.auth_token)
        self._timeout = config.timeout
        self._channel = None
        self._stub = None
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            self._ensure_stub()

    def _ensure_stub(self):
        # grpc.aio channels bind to the running event loop, so create lazily on
        # first use (inside a coroutine) rather than in __init__.
        if self._stub is None:
            import grpc

            from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2_grpc

            if not self._secure:
                self._channel = grpc.aio.insecure_channel(self._target)
            else:
                self._channel = grpc.aio.secure_channel(self._target, grpc.ssl_channel_credentials())
            self._channel.get_state(try_to_connect=True)
            self._stub = sandbox_pb2_grpc.SandboxServiceStub(self._channel)
        return self._stub

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close(None)

    def __getattr__(self, name: str) -> Any:
        rpc_name = self._methods.get(name)
        if rpc_name is not None:
            if name in _STREAMING_OUT:
                def stream_call(request, *args, **kwargs):
                    # Streaming RPCs are open-ended: long-lived (run, dial,
                    # host_port_tunnel) or caller-bounded (wait_session passes its
                    # own timeout). Never apply the client-wide unary timeout as a
                    # deadline, or a persistent tunnel/exec dies with
                    # DEADLINE_EXCEEDED after `timeout` seconds. Honor only an
                    # explicit per-call timeout.
                    timeout = kwargs.pop("timeout", None)
                    method = getattr(self._ensure_stub(), rpc_name)
                    return method(request, *args, metadata=self._metadata, timeout=timeout, **kwargs)

                return stream_call

            async def unary_call(request, *args, **kwargs):
                timeout = kwargs.pop("timeout", self._timeout)
                method = getattr(self._ensure_stub(), rpc_name)
                try:
                    return await method(request, *args, metadata=self._metadata, timeout=timeout, **kwargs)
                except Exception as err:
                    raise map_rpc_error(err) from err

            return unary_call

        rpc_name = self._ssh_methods.get(name)
        if rpc_name is None:
            raise AttributeError(name)

        async def ssh_call(request, *args, **kwargs):
            timeout = kwargs.pop("timeout", self._timeout)
            loop = asyncio.get_running_loop()
            for service in (self._SSH_SERVICE, self._SSH_LEGACY_SERVICE):
                try:
                    return await loop.run_in_executor(
                        None,
                        functools.partial(
                            connect_unary_blocking,
                            self._http_base,
                            self._metadata,
                            service,
                            rpc_name,
                            request,
                            timeout=timeout,
                        ),
                    )
                except _RouteNotFoundError:
                    continue
            raise map_rpc_error(
                _ConnectError("unimplemented", f"{rpc_name}: not served by this engine")
            )

        return ssh_call
