from __future__ import annotations

import io
import queue
import threading
from typing import Iterator

from ._pb import pb
from .errors import CapabilityUnavailableError, PrimitiveTimeoutError, map_rpc_error


class DialConn(io.RawIOBase):
    """gRPC bidi-stream backed raw IO connection for the Dial RPC."""

    def __init__(self, outbound: queue.Queue, response_iter) -> None:
        super().__init__()
        self._outbound = outbound
        self._inbound: queue.Queue[bytes | Exception | None] = queue.Queue()
        self._buf = b""
        self._eof = False
        self._done = threading.Event()
        self._pump_thread = threading.Thread(target=self._pump, args=(response_iter,), daemon=True)
        self._pump_thread.start()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def readinto(self, b: bytearray | memoryview) -> int:
        if self.closed:
            raise ValueError("read from closed connection")
        if self._eof:
            return 0
        while not self._buf:
            item = self._inbound.get()
            if item is None:
                self._eof = True
                return 0
            if isinstance(item, Exception):
                raise item
            self._buf += item
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n

    def write(self, data: bytes | bytearray | memoryview) -> int:
        if self.closed:
            raise ValueError("write to closed connection")
        if self._done.is_set():
            raise BrokenPipeError("connection closed by peer")
        if not data:
            return 0
        self._outbound.put(pb.DialRequest(data=bytes(data)))
        return len(data)

    def close(self) -> None:
        if not self.closed:
            self._outbound.put(pb.DialRequest(half_close=True))
            self._outbound.put(None)
            super().close()
            self._inbound.put(None)

    def _pump(self, response_iter) -> None:
        try:
            for frame in response_iter:
                kind = frame.WhichOneof("payload")
                if kind == "data":
                    self._inbound.put(bytes(frame.data))
                elif kind == "closed":
                    return
        except Exception as err:
            self._inbound.put(map_rpc_error(err))
        finally:
            self._done.set()
            self._inbound.put(None)
            self._outbound.put(None)


def open_dial_conn(rpc, session_id: str, unix_socket_path: str, connect_timeout_ms: int = 0) -> DialConn:
    outbound: queue.Queue[pb.DialRequest | None] = queue.Queue()
    outbound.put(pb.DialRequest(
        open=pb.DialOpen(
            session_id=session_id,
            unix_socket_path=unix_socket_path,
            connect_timeout_ms=connect_timeout_ms,
        )
    ))

    def _request_iter() -> Iterator[pb.DialRequest]:
        while True:
            item = outbound.get()
            if item is None:
                return
            yield item

    try:
        response_iter = rpc.dial(_request_iter(), timeout=None)
        first = next(response_iter)
    except StopIteration:
        raise RuntimeError("dial stream closed before open")
    except Exception as err:
        raise map_rpc_error(err) from err

    kind = first.WhichOneof("payload")
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
    if kind != "opened":
        raise RuntimeError(f"dial: unexpected first frame: {kind}")

    return DialConn(outbound, response_iter)
