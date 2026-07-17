from __future__ import annotations

import re
import threading
import time
from datetime import datetime, timezone
from typing import Iterable

import grpc

from ._pb import pb
from ._transport import grpc_target

DATA_PLANE_CREDENTIAL_HEADER = "x-tenki-session-cert"


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
    for candidate in (err, getattr(err, "__cause__", None)):
        if isinstance(candidate, grpc.RpcError):
            if candidate.code() != grpc.StatusCode.FAILED_PRECONDITION:
                return False
            return _credential_error_state(candidate) != "PAUSED"
    return True


def _is_edge_not_ready(err) -> bool:
    code = err.code() if hasattr(err, "code") and callable(err.code) else None
    name = getattr(code, "name", str(code))
    if name == "UNIMPLEMENTED" and "404" in str(err):
        return True
    # While the edge re-applies the per-session route (e.g. right after resume),
    # Caddy can reset the TLS handshake instead of serving a plain 404.
    return name == "UNAVAILABLE" and (
        "Handshake read failed" in str(err) or "Connection reset" in str(err)
    )


class DataPlaneCredentialManager:
    def __init__(self, sandbox, credential=None) -> None:
        self._sandbox = sandbox
        self._credential = ""
        self._expires_at: datetime | None = None
        self._stopped = threading.Event()
        self._lock = threading.Lock()
        if credential is not None:
            self.set(credential)

    def current(self) -> str:
        with self._lock:
            return self._credential

    def set(self, credential) -> None:
        if credential is None or not getattr(credential, "credential", ""):
            return
        with self._lock:
            self._credential = credential.credential
            self._expires_at = (
                credential.expires_at.ToDatetime(tzinfo=timezone.utc)
                if credential.HasField("expires_at")
                else None
            )
        self._schedule()

    def stop(self) -> None:
        self._stopped.set()

    def _schedule(self) -> None:
        with self._lock:
            expires_at = self._expires_at
        if expires_at is None or self._stopped.is_set():
            return
        remaining = max(1.0, (expires_at - datetime.now(timezone.utc)).total_seconds())
        thread = threading.Thread(
            target=self._renew_loop, args=(remaining / 2,), daemon=True
        )
        thread.start()

    def _renew_loop(self, delay: float) -> None:
        if self._stopped.wait(delay):
            return
        backoff = 1.0
        while not self._stopped.is_set():
            try:
                resp = self._sandbox._client._rpc.create_session_credential(
                    pb.CreateSessionCredentialRequest(session_id=self._sandbox.id)
                )
                self.set(resp.credential)
                return
            except grpc.RpcError as err:
                if not _credential_error_is_retryable(err):
                    return
            except Exception as err:
                if not _credential_error_is_retryable(err):
                    return
            if self._stopped.wait(backoff):
                return
            backoff = min(backoff * 2, 30.0)


class GrpcDataPlaneRPC:
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

    def __init__(self, endpoint: str, credentials: DataPlaneCredentialManager) -> None:
        from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2_grpc

        target, secure = grpc_target(endpoint)
        if not secure:
            self._channel = grpc.insecure_channel(target)
        else:
            self._channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())
        self._stub = sandbox_pb2_grpc.SandboxSessionDataPlaneServiceStub(self._channel)
        self._credentials = credentials

    def close(self) -> None:
        close = getattr(self._channel, "close", None)
        if callable(close):
            close()

    def ensure_serving(self, session_id: str, *, attempts: int = 24) -> None:
        # The engine publishes the per-session edge route synchronously but Caddy
        # applies config asynchronously, so an early request can get the edge's
        # plain HTTP 404. Probe with a cheap Stat until any non-404 response
        # proves the route is serving (mirrors the Go SDK's ensureDataPlaneServing).
        last = None
        for attempt in range(attempts):
            try:
                self.stat(pb.StatRequest(session_id=session_id, path="/home/tenki"))
                return
            except grpc.RpcError as err:
                if not _is_edge_not_ready(err):
                    return
                last = err
            time.sleep(min(0.05 * (attempt + 1), 0.75))
        if last is not None:
            raise last

    def __getattr__(self, name: str):
        rpc_name = self._methods.get(name)
        if not rpc_name:
            raise AttributeError(name)
        method = getattr(self._stub, rpc_name)

        def call(request, *args, **kwargs):
            metadata = tuple(kwargs.pop("metadata", ()))
            credential = self._credentials.current().strip()
            if credential:
                metadata += ((DATA_PLANE_CREDENTIAL_HEADER, credential),)
            wrapped = _wrap_request(name, request)
            resp = method(wrapped, *args, metadata=metadata, **kwargs)
            return _unwrap_response(name, resp)

        return call


# Single source of truth for the data-plane rpc-name -> envelope message-type
# mapping, shared by the sync and async transports (aio imports the atomic
# helpers) so a new/renamed RPC is wrapped identically by both.
_STREAM_REQUEST_TYPES = {
    "run": pb.SandboxSessionDataPlaneServiceRunRequest,
    "dial": pb.SandboxSessionDataPlaneServiceDialRequest,
    "host_port_tunnel": pb.SandboxSessionDataPlaneServiceHostPortTunnelRequest,
    "write_file_stream": pb.SandboxSessionDataPlaneServiceWriteFileStreamRequest,
}
_UNARY_REQUEST_TYPES = {
    "read_file": pb.SandboxSessionDataPlaneServiceReadFileRequest,
    "write_file": pb.SandboxSessionDataPlaneServiceWriteFileRequest,
    "read_file_stream": pb.SandboxSessionDataPlaneServiceReadFileStreamRequest,
    "stat": pb.SandboxSessionDataPlaneServiceStatRequest,
    "mkdir": pb.SandboxSessionDataPlaneServiceMkdirRequest,
    "remove": pb.SandboxSessionDataPlaneServiceRemoveRequest,
    "list": pb.SandboxSessionDataPlaneServiceListRequest,
}
# RPCs whose response payload is carried on `.frame` (byte streams) vs `.response`.
_FRAME_RESPONSE = frozenset({"run", "dial", "host_port_tunnel"})


def _wrap_frame(name: str, frame):
    """Wrap a single client-stream frame in its per-RPC request envelope."""
    return _STREAM_REQUEST_TYPES[name](frame=frame)


def _wrap_unary_request(name: str, request):
    """Wrap a single unary request in its per-RPC envelope (identity if none)."""
    msg = _UNARY_REQUEST_TYPES.get(name)
    return msg(request=request) if msg is not None else request


def _unwrap_field(name: str) -> str:
    """Response field carrying the payload: `frame` for byte streams else `response`."""
    return "frame" if name in _FRAME_RESPONSE else "response"


def _wrap_request(name: str, request):
    if name in _STREAM_REQUEST_TYPES:
        return (_wrap_frame(name, frame) for frame in request)
    return _wrap_unary_request(name, request)


def _unwrap_response(name: str, response):
    if name in {"run", "dial", "host_port_tunnel", "read_file_stream"}:
        return _unwrap_stream(response, _unwrap_field(name))
    if name in {
        "write_file_stream",
        "read_file",
        "write_file",
        "stat",
        "mkdir",
        "remove",
        "list",
    }:
        return response.response
    return response


def _unwrap_stream(stream: Iterable, field: str):
    for item in stream:
        yield getattr(item, field)
