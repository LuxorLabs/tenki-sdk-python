from __future__ import annotations

import asyncio

import pytest

from tenki_sandbox import SSHUnavailableError, UnauthorizedError
from tenki_sandbox._pb import pb
from tenki_sandbox.aio.dial import AsyncDialConn, open_dial_conn
from tenki_sandbox.aio.ssh import AsyncSSHConn, open_ssh_conn
from tenki_sandbox.aio.tunnel import open_host_port_tunnel, open_resilient_host_port_tunnel


# --------------------------------------------------------------------------- SSH


class _FakeWS:
    def __init__(self, incoming, *, close_exc=None):
        self._incoming = list(incoming)
        self.sent = []
        self.closed = False
        self._close_exc = close_exc  # raised when drained; default = clean close

    async def recv(self):
        if self._incoming:
            return self._incoming.pop(0)
        import websockets

        raise self._close_exc or websockets.exceptions.ConnectionClosedOK(None, None)

    async def send(self, data):
        self.sent.append(bytes(data))

    async def close(self):
        self.closed = True


async def test_async_ssh_read_write_close(monkeypatch):
    fake = _FakeWS([b"hello", b"world"])
    captured = {}

    async def fake_connect(url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs.get("additional_headers")
        return fake

    monkeypatch.setattr("websockets.connect", fake_connect)

    conn = await open_ssh_conn(None, "sess-1", "wss://gw.example", "tk_test", gateway_address_explicit=True)
    assert isinstance(conn, AsyncSSHConn)
    assert captured["url"] == "wss://gw.example/v1/ssh/sess-1"
    assert captured["headers"]["authorization"] == "Bearer tk_test"

    assert await conn.read() == b"hello"
    assert await conn.read() == b"world"
    assert await conn.read() == b""  # EOF

    await conn.write(b"ping")
    assert fake.sent == [b"ping"]

    await conn.close()
    assert fake.closed


def test_ws_header_kwarg_selects_by_signature():
    from tenki_sandbox.aio.ssh import _ws_header_kwarg

    def modern(uri, *, additional_headers=None, max_size=None):  # websockets>=14
        ...

    def legacy(uri, *, extra_headers=None, max_size=None):  # websockets 12.x/13.x
        ...

    assert _ws_header_kwarg(modern) == "additional_headers"
    assert _ws_header_kwarg(legacy) == "extra_headers"


async def test_async_ssh_uses_extra_headers_on_legacy_websockets(monkeypatch):
    # websockets 12.x/13.x top-level connect only accepts extra_headers; passing
    # additional_headers (pre-fix behavior) would TypeError and kill async SSH.
    captured = {}
    fake = _FakeWS([b"SSH-2.0-x\r\n"])

    async def legacy_connect(url, *, extra_headers=None, max_size=None):
        captured["extra_headers"] = extra_headers
        return fake

    monkeypatch.setattr("websockets.connect", legacy_connect)

    conn = await open_ssh_conn(None, "sess-1", "wss://gw.example", "tk_test", gateway_address_explicit=True)
    assert isinstance(conn, AsyncSSHConn)
    assert captured["extra_headers"]["authorization"] == "Bearer tk_test"
    await conn.close()


async def test_async_ssh_clean_close_is_eof(monkeypatch):
    import websockets

    fake = _FakeWS([b"out"], close_exc=websockets.exceptions.ConnectionClosedOK(None, None))

    async def fake_connect(url, **kwargs):
        return fake

    monkeypatch.setattr("websockets.connect", fake_connect)
    conn = await open_ssh_conn(None, "s", "wss://gw", "tk", gateway_address_explicit=True)
    assert await conn.read() == b"out"
    assert await conn.read() == b""  # normal closure -> clean EOF


async def test_async_ssh_abnormal_close_raises(monkeypatch):
    import websockets

    # non-1000/1001 close mid-stream must surface, not look like a clean EOF.
    fake = _FakeWS([b"partial"], close_exc=websockets.exceptions.ConnectionClosedError(None, None))

    async def fake_connect(url, **kwargs):
        return fake

    monkeypatch.setattr("websockets.connect", fake_connect)
    conn = await open_ssh_conn(None, "s", "wss://gw", "tk", gateway_address_explicit=True)
    assert await conn.read() == b"partial"
    with pytest.raises(SSHUnavailableError):
        await conn.read()


async def test_async_ssh_drops_text_frames(monkeypatch):
    # A text frame from the gateway/proxy must not be injected into the binary
    # SSH stream; it is dropped like the sync client's non-BINARY opcode filter.
    fake = _FakeWS([b"bin1", "text-drop-me", b"bin2"])

    async def fake_connect(url, **kwargs):
        return fake

    monkeypatch.setattr("websockets.connect", fake_connect)

    conn = await open_ssh_conn(None, "sess-1", "wss://gw.example", "tk", gateway_address_explicit=True)
    assert await conn.read() == b"bin1"
    assert await conn.read() == b"bin2"  # text frame skipped, not b"text-drop-me"
    assert await conn.read() == b""  # EOF


async def test_async_ssh_connect_error_maps_status(monkeypatch):
    class Boom(Exception):
        def __init__(self):
            super().__init__("unauthorized")
            self.response = type("R", (), {"status_code": 401})()

    async def fake_connect(url, **kwargs):
        raise Boom()

    monkeypatch.setattr("websockets.connect", fake_connect)
    with pytest.raises(UnauthorizedError):
        await open_ssh_conn(None, "sess-1", "wss://gw.example", "tk", gateway_address_explicit=True)


# -------------------------------------------------------------------------- dial


class _DialRPC:
    def dial(self, requests, **kwargs):
        async def gen():
            first = await requests.__anext__()
            assert first.WhichOneof("payload") == "open"
            yield pb.DialResponse(opened=pb.DialOpened())
            async for frame in requests:
                kind = frame.WhichOneof("payload")
                if kind == "data":
                    yield pb.DialResponse(data=bytes(frame.data))  # echo
                elif kind == "half_close":
                    yield pb.DialResponse(closed=pb.DialClosed())
                    return

        return gen()


async def test_async_dial_round_trip():
    conn = await open_dial_conn(_DialRPC(), "sess-1", "/run/app.sock")
    assert isinstance(conn, AsyncDialConn)
    await conn.write(b"hi")
    assert await asyncio.wait_for(conn.read(), timeout=5) == b"hi"
    await conn.aclose()


async def test_async_dial_close_is_aclose_alias():
    # close() aliases aclose() for naming consistency with AsyncSSHConn/tunnel.
    assert AsyncDialConn.close is AsyncDialConn.aclose
    conn = await open_dial_conn(_DialRPC(), "sess-1", "/run/app.sock")
    await conn.close()
    assert conn._closed is True


# ------------------------------------------------------------------------ tunnel


async def _start_echo_server():
    async def handle(reader, writer):
        while True:
            data = await reader.read(1024)
            if not data:
                break
            writer.write(data)
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


class _TunnelRPC:
    def __init__(self):
        self.received = []

    def host_port_tunnel(self, requests, **kwargs):
        received = self.received

        async def gen():
            await requests.__anext__()  # open
            yield pb.HostPortTunnelResponse(
                opened=pb.HostPortTunnelOpened(sandbox_port=5555, sandbox_address="10.0.0.1:5555")
            )
            yield pb.HostPortTunnelResponse(accept=pb.HostPortTunnelAccept(sub_stream_id=1))
            yield pb.HostPortTunnelResponse(data=pb.HostPortTunnelData(sub_stream_id=1, payload=b"ping"))
            async for frame in requests:
                kind = frame.WhichOneof("payload")
                received.append(kind)
                if kind == "data":
                    received.append(bytes(frame.data.payload))
                    break
            yield pb.HostPortTunnelResponse(terminated=pb.HostPortTunnelTerminated(detail="done"))

        return gen()


async def test_async_host_port_tunnel_round_trip():
    server, port = await _start_echo_server()
    try:
        rpc = _TunnelRPC()
        tunnel = await open_host_port_tunnel(rpc, "sess-1", f"127.0.0.1:{port}")
        assert tunnel.sandbox_port == 5555
        assert tunnel.sandbox_address == "10.0.0.1:5555"

        await asyncio.wait_for(tunnel.terminated.wait(), timeout=5)
        # engine-side received the echoed "ping" back from the host connection
        assert b"ping" in rpc.received
        assert tunnel._termination is not None
        await tunnel.close()
    finally:
        server.close()
        await server.wait_closed()


class _StableTunnelRPC:
    """Opens and stays connected (no terminated frame) until the SDK closes."""

    def host_port_tunnel(self, requests, **kwargs):
        async def gen():
            await requests.__anext__()
            yield pb.HostPortTunnelResponse(
                opened=pb.HostPortTunnelOpened(sandbox_port=7777, sandbox_address="10.0.0.2:7777")
            )
            async for _ in requests:  # drains until close() feeds the None sentinel
                pass

        return gen()


async def test_async_callback_task_is_strongly_referenced_until_done():
    # asyncio only weakly references create_task results; the tunnel must hold a
    # strong ref or a not-yet-run async callback can be GC'd and never fire.
    from tenki_sandbox.aio import tunnel

    ran = asyncio.Event()

    async def cb(_):
        ran.set()

    before = len(tunnel._cb_tasks)
    tunnel._invoke_cb(cb, None)
    assert len(tunnel._cb_tasks) == before + 1  # strong ref held while pending

    await asyncio.wait_for(ran.wait(), timeout=1)
    await asyncio.sleep(0)  # let the done-callback run
    assert len(tunnel._cb_tasks) == before  # discarded on completion


async def test_async_callback_exception_is_retrieved_and_swallowed():
    # A raising async callback must not leak a "Task exception was never
    # retrieved" traceback; the done-callback retrieves it (matches sync swallow).
    from tenki_sandbox.aio import tunnel

    async def boom(_):
        raise RuntimeError("callback bug")

    before = set(tunnel._cb_tasks)
    tunnel._invoke_cb(boom, None)
    new = tunnel._cb_tasks - before
    assert len(new) == 1
    task = next(iter(new))

    await asyncio.sleep(0)
    await asyncio.sleep(0)  # let the task run + done-callback fire
    assert task.done()
    assert isinstance(task.exception(), RuntimeError)  # retrieved, no dangling warning
    assert task not in tunnel._cb_tasks  # discarded


async def test_async_resilient_tunnel_on_terminated_async_callback_fires():
    resilient = await open_resilient_host_port_tunnel(_StableTunnelRPC(), "sess-1", "127.0.0.1:1")
    fired = asyncio.Event()

    async def on_term(_term):
        fired.set()

    resilient.on_terminated(on_term)
    await resilient.close()
    await asyncio.wait_for(fired.wait(), timeout=2)


async def test_fake_tunnel_is_lightweight_terminated_standin():
    # The reconnect loop's failed-attempt stand-in must be the lightweight
    # _FakeTunnel (terminated Event + _termination), not a full AsyncHostPortTunnel.
    from tenki_sandbox.aio.tunnel import AsyncHostPortTunnel, _FakeTunnel
    from tenki_sandbox.models import TunnelTermination

    fake = _FakeTunnel(TunnelTermination(reason="transport_error"))
    fake.terminated.set()
    assert not isinstance(fake, AsyncHostPortTunnel)
    assert fake.terminated.is_set()
    assert fake._termination.reason == "transport_error"


async def test_substream_done_callback_retrieves_exception():
    # A real (non-cancelled) substream failure must be retrieved by the done
    # callback, not left dangling — matches _discard_cb_task.
    from tenki_sandbox.aio.tunnel import AsyncHostPortTunnel

    t = AsyncHostPortTunnel(sandbox_port=0, sandbox_address="", outbound=asyncio.Queue(), host_addr="x")

    async def boom():
        raise RuntimeError("substream failed")

    task = asyncio.create_task(boom())
    t._substream_tasks.add(task)
    task.add_done_callback(t._discard_substream_task)

    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert task.done() and isinstance(task.exception(), RuntimeError)  # retrieved
    assert task not in t._substream_tasks  # discarded


async def test_async_resilient_tunnel_open_and_close():
    resilient = await open_resilient_host_port_tunnel(_StableTunnelRPC(), "sess-1", "127.0.0.1:1")
    assert resilient.sandbox_port == 7777
    assert resilient.state == "connected"
    await resilient.close()
    assert resilient.terminated.is_set()
    assert resilient.state == "terminated"


async def test_async_tunnel_on_terminated_callback_fires():
    server, port = await _start_echo_server()
    try:
        tunnel = await open_host_port_tunnel(_TunnelRPC(), "sess-1", f"127.0.0.1:{port}")
        seen = asyncio.Event()
        tunnel.on_terminated(lambda term: seen.set())
        await asyncio.wait_for(seen.wait(), timeout=5)
    finally:
        server.close()
        await server.wait_closed()
