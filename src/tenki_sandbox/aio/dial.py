from __future__ import annotations

import asyncio

from .._pb import pb
from ..errors import CapabilityUnavailableError, PrimitiveTimeoutError, map_rpc_error


class AsyncDialConn:
    """Async byte stream over the Dial bidi gRPC stream (read/write/aclose)."""

    def __init__(self, outbound: asyncio.Queue, response_iter) -> None:
        self._outbound = outbound
        self._inbound: asyncio.Queue = asyncio.Queue()
        self._buf = b""
        self._eof = False
        self._closed = False
        self._done = asyncio.Event()
        self._pump_task = asyncio.create_task(self._pump(response_iter))

    async def read(self, n: int = -1) -> bytes:
        """Return up to ``n`` bytes (all buffered/next frame if n<0). ``b""`` at EOF."""
        if self._eof and not self._buf:
            return b""
        while not self._buf:
            item = await self._inbound.get()
            if item is None:
                self._eof = True
                return b""
            if isinstance(item, Exception):
                raise item
            self._buf += item
        if n is None or n < 0:
            data, self._buf = self._buf, b""
        else:
            data, self._buf = self._buf[:n], self._buf[n:]
        return data

    async def write(self, data: bytes | bytearray | memoryview) -> int:
        if self._closed:
            raise ValueError("write to closed connection")
        if self._done.is_set():
            raise BrokenPipeError("connection closed by peer")
        if not data:
            return 0
        await self._outbound.put(pb.DialRequest(data=bytes(data)))
        return len(data)

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._outbound.put(pb.DialRequest(half_close=True))
            await self._outbound.put(None)
            await self._inbound.put(None)
            if self._pump_task is not None:
                self._pump_task.cancel()

    # Alias for naming consistency with AsyncSSHConn/AsyncHostPortTunnel.close()
    # and the TS/Go SDKs; aclose() stays for the async-resource convention.
    close = aclose

    async def __aenter__(self) -> "AsyncDialConn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    def __aiter__(self) -> "AsyncDialConn":
        return self

    async def __anext__(self) -> bytes:
        chunk = await self.read()
        if not chunk:
            raise StopAsyncIteration
        return chunk

    async def _pump(self, response_iter) -> None:
        try:
            async for frame in response_iter:
                kind = frame.WhichOneof("payload")
                if kind == "data":
                    await self._inbound.put(bytes(frame.data))
                elif kind == "closed":
                    return
        except Exception as err:
            await self._inbound.put(map_rpc_error(err))
        finally:
            self._done.set()
            await self._inbound.put(None)
            await self._outbound.put(None)


async def open_dial_conn(rpc, session_id: str, unix_socket_path: str, connect_timeout_ms: int = 0) -> AsyncDialConn:
    outbound: asyncio.Queue = asyncio.Queue()
    outbound.put_nowait(
        pb.DialRequest(
            open=pb.DialOpen(
                session_id=session_id,
                unix_socket_path=unix_socket_path,
                connect_timeout_ms=connect_timeout_ms,
            )
        )
    )

    async def request_iter():
        while True:
            item = await outbound.get()
            if item is None:
                return
            yield item

    response_iter = rpc.dial(request_iter(), timeout=None).__aiter__()

    async def _abort() -> None:
        # A failed open never hands the stream to AsyncDialConn, so nothing
        # would otherwise drain request_iter or close the call — leaving the
        # gRPC request-iterator task pending. Feed the sentinel and aclose.
        outbound.put_nowait(None)
        aclose = getattr(response_iter, "aclose", None)
        if aclose is not None:
            try:
                await aclose()
            except BaseException:  # noqa: BLE001 - cleanup must not mask the open error
                pass

    try:
        first = await response_iter.__anext__()
    except StopAsyncIteration:
        await _abort()
        raise RuntimeError("dial stream closed before open")
    except Exception as err:
        await _abort()
        raise map_rpc_error(err) from err

    kind = first.WhichOneof("payload")
    if kind == "opened":
        return AsyncDialConn(outbound, response_iter)

    await _abort()
    if kind == "closed":
        reason = first.closed.reason
        detail = first.closed.detail or ""
        if reason == pb.DialClosed.REASON_CAPABILITY_UNAVAILABLE:
            if detail.startswith("first_frame_timeout"):
                raise PrimitiveTimeoutError("dial", detail)
            raise CapabilityUnavailableError("dial", detail)
        if reason == pb.DialClosed.REASON_TIMEOUT:
            raise TimeoutError(f"dial connect timed out: {detail}")
        raise RuntimeError(f"dial failed: {detail}")
    raise RuntimeError(f"dial: unexpected first frame: {kind}")
