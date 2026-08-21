from __future__ import annotations

from .._pb import ssh_pb
from ..errors import SSHUnavailableError
from ..ssh import (
    _build_gateway_ssh_url,
    _override_gateway_host,
    _raise_ws_connect_error,
    _ssh_url_from_gateways,
)

_SSH_UNAVAILABLE_MSG = (
    "Async SSH transport requires 'websockets'. "
    "Install it: pip install 'tenki[async]'"
)


class AsyncSSHConn:
    """WebSocket-backed async SSH transport.

    Exposes a raw async byte stream (``read``/``write``/``close``), matching the
    TS and Go SDKs. Pair with an async SSH client; the sync ``SSHConn`` (paramiko
    adapter) is a separate, synchronous concern.
    """

    def __init__(self, ws) -> None:
        self._ws = ws
        self._buf = b""
        self._eof = False
        self._closed = False

    async def read(self, n: int = -1) -> bytes:
        """Return up to ``n`` bytes (all buffered/next frame if n<0).

        Returns ``b""`` at a clean end of stream. Raises ``SSHUnavailableError`` if
        the WebSocket drops abnormally (transport error or non-1000/1001 close), so
        a truncated read is distinguishable from a clean end — mirroring Go's
        ``mapSSHReadError`` and ``AsyncDialConn``.
        """
        import websockets

        if self._eof and not self._buf:
            return b""
        while not self._buf:
            try:
                message = await self._ws.recv()
            except websockets.exceptions.ConnectionClosedOK:
                self._eof = True  # normal closure (1000/1001) == EOF
                return b""
            except Exception as err:
                self._eof = True
                raise SSHUnavailableError(f"ssh stream closed abnormally: {err}") from err
            # SSH is a binary transport; drop text frames so a gateway/proxy
            # control message can't desync the stream (mirrors sync SSHConn,
            # which only keeps _OPCODE_BINARY frames). websockets returns str for
            # text frames and bytes for binary.
            if isinstance(message, str):
                continue
            self._buf += bytes(message)
        if n is None or n < 0:
            data, self._buf = self._buf, b""
        else:
            data, self._buf = self._buf[:n], self._buf[n:]
        return data

    async def write(self, data: bytes | bytearray | memoryview) -> int:
        if not data:
            return 0
        payload = bytes(data)
        await self._ws.send(payload)
        return len(payload)

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                await self._ws.close()
            except Exception:
                pass

    async def __aenter__(self) -> "AsyncSSHConn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def __aiter__(self) -> "AsyncSSHConn":
        return self

    async def __anext__(self) -> bytes:
        chunk = await self.read()
        if not chunk:
            raise StopAsyncIteration
        return chunk


async def _discover_ssh_gateway(rpc, session_id: str) -> str:
    """ListActiveSSHGateways -> WS URL of first healthy bridged gateway, or ""."""
    if not session_id.strip():
        return ""
    try:
        resp = await rpc.list_active_ssh_gateways(ssh_pb.ListActiveSSHGatewaysRequest(session_id=session_id))
    except Exception:
        return ""
    return _ssh_url_from_gateways(resp.gateways, session_id)


def _extract_status(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    if response is not None and getattr(response, "status_code", None) is not None:
        return response.status_code
    return getattr(exc, "status_code", None)


def _ws_header_kwarg(connect) -> str:
    """Header kwarg accepted by the installed ``websockets.connect``.

    websockets>=14 renamed it from ``extra_headers`` (12.x/13.x) to
    ``additional_headers`` on the top-level connect. Detect it so the declared
    floor (``websockets>=12``) actually works instead of raising TypeError.
    """
    import inspect

    try:
        params = inspect.signature(connect).parameters
    except (TypeError, ValueError):
        return "additional_headers"
    if "additional_headers" in params:
        return "additional_headers"
    if "extra_headers" in params:
        return "extra_headers"
    return "additional_headers"


async def open_ssh_conn(
    rpc,
    session_id: str,
    gateway_url: str,
    auth_token: str,
    *,
    override_gateway_url: str | None = None,
    gateway_address_explicit: bool = False,
) -> AsyncSSHConn:
    """Build the WS URL, dial, return AsyncSSHConn. Raises SSHUnavailableError on failure."""
    try:
        import websockets
    except ImportError:
        raise SSHUnavailableError(_SSH_UNAVAILABLE_MSG)

    ws_url = _build_gateway_ssh_url(gateway_url, session_id)

    if not gateway_address_explicit and not (override_gateway_url or "").strip():
        discovered = await _discover_ssh_gateway(rpc, session_id)
        if discovered:
            ws_url = discovered

    if (override_gateway_url or "").strip():
        ws_url = _override_gateway_host(ws_url, override_gateway_url, session_id)

    if not ws_url:
        raise SSHUnavailableError("gateway address not configured")

    from .._transport import auth_metadata as _auth_meta

    headers = {k: v for k, v in _auth_meta(auth_token)}
    header_kwarg = _ws_header_kwarg(websockets.connect)

    try:
        ws = await websockets.connect(ws_url, max_size=None, **{header_kwarg: headers})
    except Exception as exc:
        _raise_ws_connect_error(_extract_status(exc), str(exc))

    return AsyncSSHConn(ws)
