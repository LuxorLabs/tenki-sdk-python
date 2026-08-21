from __future__ import annotations

import io
import threading
from urllib.parse import urlparse

from ._pb import ssh_pb
from .errors import (
    InvalidStateError,
    PermissionDeniedError,
    SSHUnavailableError,
    SessionNotFoundError,
    UnauthorizedError,
)

# WebSocket opcode constants
_OPCODE_BINARY = 2
_OPCODE_CLOSE = 8

_SSH_UNAVAILABLE_MSG = (
    "SSH transport requires 'websocket-client'. "
    "Install it: pip install websocket-client"
)


class SSHConn(io.RawIOBase):
    """WebSocket-backed SSH transport. Implements io.RawIOBase for paramiko compatibility."""

    def __init__(self, ws) -> None:
        super().__init__()
        self._ws = ws
        self._read_mu = threading.Lock()
        self._write_mu = threading.Lock()
        self._buf = b""

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def readinto(self, b: bytearray | memoryview) -> int:
        if not b:
            return 0
        with self._read_mu:
            while not self._buf:
                try:
                    opcode, data = self._ws.recv_data()
                except Exception:
                    return 0
                if opcode == _OPCODE_CLOSE or data is None:
                    return 0
                if opcode == _OPCODE_BINARY:
                    self._buf += bytes(data)
            n = min(len(b), len(self._buf))
            b[:n] = self._buf[:n]
            self._buf = self._buf[n:]
            return n

    def write(self, b: bytes | bytearray | memoryview) -> int:
        if not b:
            return 0
        data = bytes(b)
        with self._write_mu:
            self._ws.send_binary(data)
        return len(data)

    @property
    def _closed(self) -> bool:
        # paramiko compat: it checks sock._closed (private) not sock.closed
        return self.closed

    def settimeout(self, timeout) -> None:
        # paramiko compat: WS transport handles its own timeouts
        pass

    def recv(self, bufsize: int) -> bytes:
        buf = bytearray(bufsize)
        n = self.readinto(buf)
        return bytes(buf[:n])

    def send(self, data: bytes) -> int:
        return self.write(data)

    def close_write(self) -> None:
        # WebSocket has no half-close; no-op (matches Go SDK)
        pass

    def close(self) -> None:
        if not self.closed:
            try:
                with self._write_mu:
                    self._ws.close()
            except Exception:
                pass
            super().close()


def _build_gateway_ssh_url(gateway_url: str, session_id: str) -> str:
    """Append /v1/ssh/<session_id> and normalize scheme to ws/wss."""
    gw = gateway_url.strip().rstrip("/")
    if not gw:
        return ""
    parsed = urlparse(gw if "://" in gw else "wss://" + gw)
    scheme = parsed.scheme
    if scheme == "http":
        scheme = "ws"
    elif scheme not in ("ws", "wss"):
        scheme = "wss"
    path = parsed.path.rstrip("/") + "/v1/ssh/" + session_id.strip()
    return f"{scheme}://{parsed.netloc}{path}"


def _override_gateway_host(base: str, override: str, session_id: str) -> str:
    """Replace scheme+host of base with override. If base is empty, build a fresh URL."""
    ou = urlparse(override if "://" in override else "wss://" + override.strip())
    if not ou.netloc:
        return base
    scheme = ou.scheme
    if scheme == "http":
        scheme = "ws"
    elif scheme not in ("ws", "wss"):
        scheme = "wss"
    if not base:
        return f"{scheme}://{ou.netloc}/v1/ssh/{session_id.strip()}"
    bu = urlparse(base)
    return f"{scheme}://{ou.netloc}{bu.path}"


def _ssh_url_from_gateways(gateways, session_id: str) -> str:
    """WS URL of the first healthy bridged gateway, or "".

    Pure URL shaping, shared by the sync and async _discover_ssh_gateway so the
    gateway-URL format lives in one place (per the async SDK contract).
    """
    for gw in gateways:
        bridge = (gw.ws_bridge_endpoint or "").strip()
        if not bridge or not gw.healthy:
            continue
        parsed = urlparse(bridge if "://" in bridge else "wss://" + bridge)
        if not parsed.netloc:
            continue
        scheme = parsed.scheme
        if scheme == "http":
            scheme = "ws"
        elif scheme not in ("ws", "wss"):
            scheme = "wss"
        path = parsed.path.rstrip("/") + "/v1/ssh/" + session_id.strip()
        return f"{scheme}://{parsed.netloc}{path}"
    return ""


def _discover_ssh_gateway(rpc, session_id: str) -> str:
    """Call ListActiveSSHGateways -> WS URL of first healthy bridged gateway, or ""."""
    if not session_id.strip():
        return ""
    try:
        resp = rpc.list_active_ssh_gateways(ssh_pb.ListActiveSSHGatewaysRequest(session_id=session_id))
    except Exception:
        return ""
    return _ssh_url_from_gateways(resp.gateways, session_id)


def open_ssh_conn(
    rpc,
    session_id: str,
    gateway_url: str,
    auth_token: str,
    *,
    override_gateway_url: str | None = None,
    gateway_address_explicit: bool = False,
) -> SSHConn:
    """Build the WS URL, dial, return SSHConn. Raises SSHUnavailableError on failure."""
    try:
        import websocket as _ws_lib
    except ImportError:
        raise SSHUnavailableError(_SSH_UNAVAILABLE_MSG)

    ws_url = _build_gateway_ssh_url(gateway_url, session_id)

    if not gateway_address_explicit and not (override_gateway_url or "").strip():
        discovered = _discover_ssh_gateway(rpc, session_id)
        if discovered:
            ws_url = discovered

    if (override_gateway_url or "").strip():
        ws_url = _override_gateway_host(ws_url, override_gateway_url, session_id)

    if not ws_url:
        raise SSHUnavailableError("gateway address not configured")

    from ._transport import auth_metadata as _auth_meta
    headers = {k: v for k, v in _auth_meta(auth_token)}

    ws = _ws_lib.WebSocket()
    try:
        ws.connect(ws_url, header=headers, suppress_origin=True)
    except Exception as exc:
        status = getattr(exc, "status_code", None)
        _raise_ws_connect_error(status, str(exc))

    ws.settimeout(None)
    return SSHConn(ws)


def _raise_ws_connect_error(status_code: int | None, msg: str) -> None:
    if status_code == 401:
        raise UnauthorizedError(msg)
    if status_code == 403:
        raise PermissionDeniedError(msg)
    if status_code == 404:
        raise SessionNotFoundError(msg)
    if status_code == 409:
        raise InvalidStateError(msg)
    raise SSHUnavailableError(msg)
