"""Tests for dial.py — DialConn and open_dial_conn."""
from __future__ import annotations

import queue
import threading

import pytest

from tenki_sandbox._pb import pb
from tenki_sandbox.dial import DialConn, open_dial_conn
from tenki_sandbox.errors import CapabilityUnavailableError, PrimitiveTimeoutError, SandboxError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn_no_thread(frames):
    """Build a DialConn without starting its pump thread; returns (conn, outbound)."""
    outbound = queue.Queue()
    response_iter = iter(frames)

    conn = DialConn.__new__(DialConn)
    # Call io.RawIOBase.__init__ manually
    import io
    io.RawIOBase.__init__(conn)
    conn._outbound = outbound
    conn._inbound = queue.Queue()
    conn._buf = b""
    conn._eof = False
    conn._done = threading.Event()
    # Do NOT start pump thread — tests call _pump directly when needed
    return conn, outbound


class FakeRPC:
    """Minimal RPC that returns a canned iterator for dial()."""

    def __init__(self, frames):
        self._frames = frames
        self.requests = []

    def dial(self, request_iter, timeout=None):
        # Consume request_iter in background so the generator doesn't block
        t = threading.Thread(target=lambda: list(request_iter), daemon=True)
        t.start()
        return iter(self._frames)


class ErrorRPC:
    """RPC that raises on dial()."""

    def __init__(self, err):
        self._err = err

    def dial(self, request_iter, timeout=None):
        raise self._err


# ---------------------------------------------------------------------------
# open_dial_conn: happy path
# ---------------------------------------------------------------------------

def test_open_dial_conn_happy_path():
    frames = [
        pb.DialResponse(opened=pb.DialOpened(sandbox_address="127.0.0.1:1234")),
    ]
    rpc = FakeRPC(frames)
    conn = open_dial_conn(rpc, "sess-1", "/tmp/sock.sock")
    assert isinstance(conn, DialConn)
    conn.close()


def test_open_dial_conn_sends_open_frame():
    outbound_frames = []
    frames = [pb.DialResponse(opened=pb.DialOpened(sandbox_address=""))]

    class RecordingRPC:
        def dial(self, request_iter, timeout=None):
            # grab open frame
            for req in request_iter:
                outbound_frames.append(req)
                break
            return iter(frames)

    rpc = RecordingRPC()
    conn = open_dial_conn(rpc, "sess-2", "/var/run/app.sock", connect_timeout_ms=500)
    assert len(outbound_frames) == 1
    open_frame = outbound_frames[0]
    assert open_frame.WhichOneof("payload") == "open"
    assert open_frame.open.session_id == "sess-2"
    assert open_frame.open.unix_socket_path == "/var/run/app.sock"
    assert open_frame.open.connect_timeout_ms == 500
    conn.close()


# ---------------------------------------------------------------------------
# open_dial_conn: first frame error cases
# ---------------------------------------------------------------------------

def test_open_dial_conn_capability_unavailable():
    frames = [
        pb.DialResponse(
            closed=pb.DialClosed(
                reason=pb.DialClosed.REASON_CAPABILITY_UNAVAILABLE,
                detail="not supported",
            )
        )
    ]
    rpc = FakeRPC(frames)
    with pytest.raises(CapabilityUnavailableError):
        open_dial_conn(rpc, "sess", "/sock")


def test_open_dial_conn_primitive_timeout():
    frames = [
        pb.DialResponse(
            closed=pb.DialClosed(
                reason=pb.DialClosed.REASON_CAPABILITY_UNAVAILABLE,
                detail="first_frame_timeout: no guest agent response",
            )
        )
    ]
    rpc = FakeRPC(frames)
    with pytest.raises(PrimitiveTimeoutError):
        open_dial_conn(rpc, "sess", "/sock")


def test_open_dial_conn_timeout_reason():
    frames = [
        pb.DialResponse(
            closed=pb.DialClosed(
                reason=pb.DialClosed.REASON_TIMEOUT,
                detail="timed out",
            )
        )
    ]
    rpc = FakeRPC(frames)
    with pytest.raises(TimeoutError, match="dial connect timed out"):
        open_dial_conn(rpc, "sess", "/sock")


def test_open_dial_conn_other_closed_reason():
    frames = [
        pb.DialResponse(
            closed=pb.DialClosed(
                reason=pb.DialClosed.REASON_REFUSED,
                detail="connection refused",
            )
        )
    ]
    rpc = FakeRPC(frames)
    with pytest.raises(RuntimeError, match="dial failed"):
        open_dial_conn(rpc, "sess", "/sock")


def test_open_dial_conn_unexpected_first_frame():
    frames = [
        pb.DialResponse(data=b"unexpected"),
    ]
    rpc = FakeRPC(frames)
    with pytest.raises(RuntimeError, match="unexpected first frame"):
        open_dial_conn(rpc, "sess", "/sock")


def test_open_dial_conn_stream_empty():
    rpc = FakeRPC([])
    with pytest.raises(RuntimeError, match="closed before open"):
        open_dial_conn(rpc, "sess", "/sock")


def test_open_dial_conn_rpc_raises():
    class GrpcError(Exception):
        def code(self):
            class C:
                name = "UNAUTHENTICATED"
            return C()
        def details(self):
            return "token expired"

    rpc = ErrorRPC(GrpcError())
    with pytest.raises(SandboxError):
        open_dial_conn(rpc, "sess", "/sock")


# ---------------------------------------------------------------------------
# DialConn: readable / writable
# ---------------------------------------------------------------------------

def test_dial_conn_readable_writable():
    conn, _ = _make_conn_no_thread([])
    assert conn.readable() is True
    assert conn.writable() is True


# ---------------------------------------------------------------------------
# DialConn: write
# ---------------------------------------------------------------------------

def test_dial_conn_write_puts_data_frame():
    conn, outbound = _make_conn_no_thread([])
    n = conn.write(b"hello")
    assert n == 5
    frame = outbound.get_nowait()
    assert frame.WhichOneof("payload") == "data"
    assert bytes(frame.data) == b"hello"


def test_dial_conn_write_empty_returns_zero():
    conn, outbound = _make_conn_no_thread([])
    n = conn.write(b"")
    assert n == 0
    assert outbound.empty()


def test_dial_conn_write_memoryview():
    conn, outbound = _make_conn_no_thread([])
    data = bytearray(b"world")
    n = conn.write(memoryview(data))
    assert n == 5
    frame = outbound.get_nowait()
    assert bytes(frame.data) == b"world"


# ---------------------------------------------------------------------------
# DialConn: readinto
# ---------------------------------------------------------------------------

def test_dial_conn_readinto_from_inbound():
    conn, _ = _make_conn_no_thread([])
    conn._inbound.put(b"abc")
    buf = bytearray(10)
    n = conn.readinto(buf)
    assert n == 3
    assert bytes(buf[:n]) == b"abc"


def test_dial_conn_readinto_leftover_bytes():
    conn, _ = _make_conn_no_thread([])
    conn._inbound.put(b"abcde")
    buf = bytearray(3)
    n = conn.readinto(buf)
    assert n == 3
    assert bytes(buf) == b"abc"
    # second call uses leftover
    buf2 = bytearray(10)
    n2 = conn.readinto(buf2)
    assert n2 == 2
    assert bytes(buf2[:n2]) == b"de"


def test_dial_conn_readinto_closed_raises():
    conn, _ = _make_conn_no_thread([])
    conn.close()
    buf = bytearray(10)
    with pytest.raises(ValueError, match="closed"):
        conn.readinto(buf)


def test_dial_conn_readinto_none_is_eof():
    conn, _ = _make_conn_no_thread([])
    conn._inbound.put(None)
    buf = bytearray(10)
    n = conn.readinto(buf)
    assert n == 0


def test_dial_conn_readinto_exception_raises():
    conn, _ = _make_conn_no_thread([])
    conn._inbound.put(RuntimeError("boom"))
    buf = bytearray(10)
    with pytest.raises(RuntimeError, match="boom"):
        conn.readinto(buf)


# ---------------------------------------------------------------------------
# DialConn: close
# ---------------------------------------------------------------------------

def test_dial_conn_close_sends_half_close_and_none():
    conn, outbound = _make_conn_no_thread([])
    conn.close()
    frames = []
    while not outbound.empty():
        frames.append(outbound.get_nowait())
    kinds = [f.WhichOneof("payload") if not isinstance(f, type(None)) else None for f in frames]
    assert "half_close" in kinds
    assert None in kinds


def test_dial_conn_close_puts_none_in_inbound():
    conn, _ = _make_conn_no_thread([])
    conn.close()
    item = conn._inbound.get_nowait()
    assert item is None


def test_dial_conn_close_idempotent():
    conn, outbound = _make_conn_no_thread([])
    conn.close()
    # drain
    while not outbound.empty():
        outbound.get_nowait()
    while not conn._inbound.empty():
        conn._inbound.get_nowait()
    # second close should be a no-op
    conn.close()
    assert outbound.empty()


# ---------------------------------------------------------------------------
# DialConn: _pump
# ---------------------------------------------------------------------------

def test_pump_data_frame_puts_bytes_in_inbound():
    conn, _ = _make_conn_no_thread([])
    frames = [pb.DialResponse(data=b"chunk1"), pb.DialResponse(data=b"chunk2")]
    conn._pump(iter(frames))
    items = []
    while not conn._inbound.empty():
        items.append(conn._inbound.get_nowait())
    # last item is None (sentinel from finally)
    assert items[0] == b"chunk1"
    assert items[1] == b"chunk2"
    assert items[-1] is None


def test_pump_closed_frame_terminates():
    conn, _ = _make_conn_no_thread([])
    frames = [
        pb.DialResponse(data=b"before"),
        pb.DialResponse(closed=pb.DialClosed(reason=pb.DialClosed.REASON_CLEAN)),
        pb.DialResponse(data=b"after"),  # should not be processed
    ]
    conn._pump(iter(frames))
    items = []
    while not conn._inbound.empty():
        items.append(conn._inbound.get_nowait())
    data_items = [i for i in items if isinstance(i, bytes)]
    assert data_items == [b"before"]  # "after" not delivered
    assert conn._done.is_set()


def test_pump_exception_puts_mapped_error_in_inbound():
    conn, _ = _make_conn_no_thread([])

    def bad_iter():
        raise RuntimeError("network fail")
        yield  # make it a generator

    conn._pump(bad_iter())
    item = conn._inbound.get_nowait()
    assert isinstance(item, Exception)
    assert conn._done.is_set()


def test_pump_finally_puts_none_in_outbound():
    conn, outbound = _make_conn_no_thread([])
    conn._pump(iter([]))
    # outbound should get None
    item = outbound.get_nowait()
    assert item is None
